#!/bin/sh
# Fails if the engine names, outside the runtime shim, a clock, a timer, a task spawner or a
# thread that panics or never completes in a browser (wasm32-unknown-unknown).
#
# The shim is src/rt.rs. Everything else in src/ goes through it: rt::spawn, rt::sleep,
# rt::timeout, rt::yield_now, rt::run_blocking, rt::Instant, rt::now_unix_ns. A call that is
# not through the shim compiles for a browser and then traps or hangs the instance at run time,
# where nothing in CI sees it, so it is refused here instead.
#
# What it refuses (in code that is compiled into a browser build):
#   tokio::spawn, tokio::task::*, tokio::time, tokio::runtime::* (Handle, Builder), `.block_on(`,
#   Handle::current and try_current, build_global, spawn_blocking
#   std::time::{Instant, SystemTime, UNIX_EPOCH}, bare SystemTime and UNIX_EPOCH
#   affinitypool::*
#   thread::{spawn, sleep, yield_now, park, park_timeout, scope, Builder, current},
#   available_parallelism
#
# What it does not look at:
#   src/rt.rs, src/test/**
#   an item behind a #[cfg(..)] that is false in a browser build that is not a test build: the
#   attribute is evaluated (all, any, not, test, unix, windows, target_arch, target_os,
#   target_family, target_pointer_width; anything else, a feature for one, is unknown and does not
#   exempt). So `#[cfg(test)]`, `#[cfg(all(test, ..))]`, `#[cfg(not(target_arch = "wasm32"))]`
#   and `#[cfg(unix)]` exempt the item (a block, a statement, a `use`, a module and the file of a
#   `mod x;`) and `#[cfg(any(test, target_arch = "wasm32"))]` does not.
#   comments and string literals.
#
# Usage:
#   scripts/check-rt-guard.sh              check the src/ of this repository
#   scripts/check-rt-guard.sh DIR          check DIR/src (a copy of the layout, for the self-test)
#   scripts/check-rt-guard.sh --self-test  run the script against fixtures that must fail and
#                                          fixtures that must pass
#
# POSIX sh and awk only (it runs on macOS, where awk is BSD awk, and on Linux, where it is mawk or
# gawk).
set -eu

# The awk program, written out once so the two passes share the cfg evaluator.
write_awk() {
	dir=$1
	cat > "$dir/lib.awk" <<'AWK'
# --- cfg evaluation ------------------------------------------------------------------------
# A predicate is true (T), false (F) or unknown (U) for `cargo build --target
# wasm32-unknown-unknown` without `--test`. Only F exempts an item.
function tokenize(s,    i, c, j, n) {
	ntok = 0
	n = length(s)
	i = 1
	while (i <= n) {
		c = substr(s, i, 1)
		if (c == " " || c == "\t") { i++; continue }
		if (c ~ /[A-Za-z_]/) {
			j = i
			while (j <= n && substr(s, j, 1) ~ /[A-Za-z0-9_]/) j++
			tok[++ntok] = substr(s, i, j - i)
			i = j
			continue
		}
		if (c == "\"") {
			j = i + 1
			while (j <= n && substr(s, j, 1) != "\"") j++
			tok[++ntok] = substr(s, i, j - i + 1)
			i = j + 1
			continue
		}
		tok[++ntok] = c
		i++
	}
	tok[ntok + 1] = ""
}
function ev_bare(name) {
	if (name == "test" || name == "unix" || name == "windows" || name == "doctest") return "F"
	return "U"
}
function ev_kv(name, val) {
	if (name == "target_arch") return (val == "wasm32") ? "T" : "F"
	if (name == "target_os") return (val == "unknown") ? "T" : "F"
	if (name == "target_family") return (val == "wasm") ? "T" : "F"
	if (name == "target_pointer_width") return (val == "32") ? "T" : "F"
	if (name == "target_endian") return (val == "little") ? "T" : "F"
	if (name == "target_env") return (val == "") ? "T" : "F"
	if (name == "target_feature" && val == "atomics") return "F"
	return "U"
}
function ev_pred(    name, val, r, acc) {
	name = tok[pos++]
	if (tok[pos] == "(") {
		pos++
		if (name == "not") {
			r = ev_pred()
			if (tok[pos] == ")") pos++
			if (r == "T") return "F"
			if (r == "F") return "T"
			return "U"
		}
		acc = (name == "all") ? "T" : "F"
		while (tok[pos] != ")" && tok[pos] != "") {
			r = ev_pred()
			if (name == "all") {
				if (r == "F") acc = "F"
				else if (r == "U" && acc != "F") acc = "U"
			} else {
				if (r == "T") acc = "T"
				else if (r == "U" && acc != "T") acc = "U"
			}
			if (tok[pos] == ",") pos++
		}
		if (tok[pos] == ")") pos++
		if (name != "all" && name != "any") return "U"
		return acc
	}
	if (tok[pos] == "=") {
		pos++
		val = tok[pos++]
		gsub(/"/, "", val)
		return ev_kv(name, val)
	}
	return ev_bare(name)
}
# The value of the predicate of a whole `#[cfg(..)]` or `#![cfg(..)]` line, "" if it is not one.
function cfg_value(line,    inner) {
	if (line !~ /^#!?\[cfg\(.*\)\]$/) return ""
	inner = line
	sub(/^#!?\[cfg\(/, "", inner)
	sub(/\)\]$/, "", inner)
	tokenize(inner)
	pos = 1
	return ev_pred()
}
function trim(s) {
	sub(/^[ \t]+/, "", s)
	sub(/[ \t]+$/, "", s)
	return s
}
# The line with its comments and string and character literals taken out.
function strip(s) {
	gsub(/"([^"\\]|\\.)*"/, "\"\"", s)
	gsub(/'[^'\\]'/, "''", s)
	gsub(/'\\.'/, "''", s)
	sub(/\/\/.*$/, "", s)
	return s
}
AWK
	cat > "$dir/mods.awk" <<'AWK'
# Prints "parent file|module" for every `mod x;` that sits under a cfg that is false in a browser
# build, and "FILE|" for a file whose own `#![cfg(..)]` is false.
{
	raw = trim($0)
	if (raw ~ /^#!\[cfg\(/ && cfg_value(raw) == "F") { print FILENAME "|"; exit }
	if (raw ~ /^#\[cfg\(/ && cfg_value(raw) == "F") { exempt = 1; next }
	if (raw == "" || raw ~ /^#\[/ || raw ~ /^\/\//) next
	if (exempt && raw ~ /^(pub(\([a-z]+\))? )?mod [A-Za-z0-9_]+;/) {
		name = raw
		sub(/^(pub(\([a-z]+\))? )?mod /, "", name)
		sub(/;.*$/, "", name)
		print FILENAME "|" name
	}
	exempt = 0
}
AWK
	cat > "$dir/scan.awk" <<'AWK'
function report(what, text) {
	printf "%s:%d: %s: %s\n", FILENAME, startline, what, trim(text)
	bad = 1
}
function check(s) {
	if (s ~ /(^|[^A-Za-z0-9_])tokio::(spawn|task::|time|runtime::)/ || s ~ /(^|[^A-Za-z0-9_])tokio::\{[^}]*(spawn|task|time|runtime)[^A-Za-z0-9_]/ || s ~ /(^|[^A-Za-z0-9_])spawn_blocking([^A-Za-z0-9_]|$)/)
		report("tokio spawn, task, time or runtime outside src/rt.rs: use rt::spawn, rt::sleep, rt::timeout, rt::yield_now", s)
	if (s ~ /(^|[^A-Za-z0-9_])Handle::(current|try_current)/ || s ~ /\.block_on\(/ || s ~ /build_global/)
		report("a runtime handle, block_on or a global pool: traps or never completes in a browser", s)
	if (s ~ /std::time::(Instant|SystemTime|UNIX_EPOCH|\*)/ || s ~ /std::time::\{[^}]*(Instant|SystemTime|UNIX_EPOCH|\*)/ || s ~ /(^|[^A-Za-z0-9_])(SystemTime|UNIX_EPOCH)([^A-Za-z0-9_]|$)/)
		report("a std clock outside src/rt.rs: use rt::Instant and rt::now_unix_ns", s)
	if (s ~ /(^|[^A-Za-z0-9_])affinitypool::/)
		report("affinitypool outside src/rt.rs: use rt::run_blocking", s)
	if (s ~ /(^|[^A-Za-z0-9_])thread::(spawn|sleep|yield_now|park|park_timeout|park_timeout_ms|scope|Builder|current)([^A-Za-z0-9_]|$)/ || s ~ /available_parallelism/)
		report("a thread call that panics or blocks in a browser", s)
}
# Moves the end of a skipped item along by one line; the item ends where its braces close, or at a
# `;` that is outside every brace and parenthesis, or at the brace that closes its parent.
function advance(code, t,    n, m, p, q) {
	n = gsub(/\{/, "{", code); m = gsub(/\}/, "}", code)
	p = gsub(/\(/, "(", code); q = gsub(/\)/, ")", code)
	if (n > 0) seen = 1
	depth += n - m
	pdepth += p - q
	if (seen && depth <= 0) skipping = 0
	else if (!seen && depth == 0 && pdepth <= 0 && t ~ /;$/) skipping = 0
	else if (depth < 0) skipping = 0
}
BEGIN { skipping = 0; pending = 0; acc = ""; bad = 0 }
FNR == 1 { skipping = 0; pending = 0; acc = ""; depth = 0; pdepth = 0; seen = 0 }
{
	raw = trim($0)
	code = strip($0)
	t = trim(code)

	if (skipping) {
		advance(code, t)
		next
	}
	if (pending) {
		if (t == "" || t ~ /^#\[/) next
		pending = 0
		skipping = 1
		depth = 0
		pdepth = 0
		seen = 0
		advance(code, t)
		next
	}
	if (raw ~ /^#!?\[cfg\(/ && cfg_value(raw) == "F") {
		pending = 1
		next
	}
	# A `use` is read whole, so that a list over several lines is one statement.
	if (acc != "") {
		acc = acc " " t
		if (t ~ /;/) { startline = accline; check(acc); acc = "" }
		next
	}
	if (t ~ /^(pub(\([a-z]+\))? )?use / && t !~ /;/) {
		acc = t
		accline = FNR
		next
	}
	startline = FNR
	check(t)
}
END { exit bad ? 1 : 0 }
AWK
}

# Scans $1/src and prints the violations with paths relative to $1. A subshell, so the cd stays.
scan_tree() (
	cd "$1"
	root=.
	work=$(mktemp -d)
	write_awk "$work"

	find "$root/src" -name '*.rs' | sort > "$work/all"

	# Files that are not part of a browser build: src/rt.rs, src/test/, and each `mod x;` under a
	# false cfg, as a file and as a directory.
	: > "$work/excluded"
	{
		echo "$root/src/rt.rs"
		echo "$root/src/test/"
	} >> "$work/excluded"
	while IFS= read -r file; do
		awk -f "$work/lib.awk" -f "$work/mods.awk" "$file"
	done < "$work/all" > "$work/mods"
	while IFS='|' read -r parent module; do
		if [ -z "$module" ]; then
			echo "$parent" >> "$work/excluded"
			continue
		fi
		parent_dir=$(dirname "$parent")
		parent_base=$(basename "$parent" .rs)
		case "$parent_base" in
			mod | lib | main) module_dir=$parent_dir ;;
			*) module_dir=$parent_dir/$parent_base ;;
		esac
		echo "$module_dir/$module.rs" >> "$work/excluded"
		echo "$module_dir/$module/" >> "$work/excluded"
	done < "$work/mods"

	: > "$work/files"
	while IFS= read -r file; do
		skip=0
		while IFS= read -r ex; do
			case "$ex" in
				*/) case "$file" in "$ex"*) skip=1 ;; esac ;;
				*) [ "$file" = "$ex" ] && skip=1 ;;
			esac
			[ "$skip" = 1 ] && break
		done < "$work/excluded"
		[ "$skip" = 0 ] && echo "$file" >> "$work/files"
	done < "$work/all"

	status=0
	while IFS= read -r file; do
		awk -f "$work/lib.awk" -f "$work/scan.awk" "$file" || status=1
	done < "$work/files"
	rm -rf "$work"
	return $status
)

# --- self-test ------------------------------------------------------------------------------

# Builds a tree whose src/x.rs is "$1" and says whether the scan fails ("fail") or passes ("pass").
verdict() {
	fixture=$(mktemp -d)
	mkdir -p "$fixture/src"
	printf '%s\n' "$1" > "$fixture/src/x.rs"
	if scan_tree "$fixture" > /dev/null; then echo pass; else echo fail; fi
	rm -rf "$fixture"
}

expect() {
	want=$1
	snippet=$2
	got=$(verdict "$snippet")
	if [ "$got" != "$want" ]; then
		echo "self-test: expected $want, got $got, for:" >&2
		printf '%s\n' "$snippet" | sed 's/^/    /' >&2
		failures=$((failures + 1))
	fi
}

self_test() {
	failures=0

	# Each of these is a call that traps or hangs a browser, and must be refused.
	expect fail 'fn f() { tokio::spawn(async {}); }'
	expect fail 'fn f() { let _ = tokio::task::spawn_blocking(|| 1); }'
	expect fail 'use tokio::task::JoinHandle;'
	expect fail 'fn f() { tokio::task::yield_now(); }'
	expect fail 'fn f() { tokio::time::sleep(d); }'
	expect fail 'use tokio::time;'
	expect fail 'use tokio::{sync::Notify, time};'
	expect fail 'fn f() { let _ = tokio::runtime::Handle::try_current(); }'
	expect fail 'fn f() { let h = Handle::current(); }'
	expect fail 'fn f() { rt.block_on(fut); }'
	expect fail 'fn f() { Threadpool::build(1).build_global(); }'
	expect fail 'fn f() { spawn_blocking(|| 1); }'
	expect fail 'fn f() { let _ = std::time::Instant::now(); }'
	expect fail 'use std::time::Instant;'
	expect fail 'use std::time::{Duration, Instant};'
	expect fail 'use std::time::{
	Duration,
	SystemTime,
};'
	expect fail 'fn f() { let _ = SystemTime::now(); }'
	expect fail 'fn f() { let d = now.duration_since(UNIX_EPOCH); }'
	expect fail 'fn f() { let _ = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH); }'
	expect fail 'fn f() { affinitypool::spawn(|| 1).await; }'
	expect fail 'fn f() { std::thread::spawn(|| {}); }'
	expect fail 'fn f() { thread::sleep(d); }'
	expect fail 'fn f() { std::thread::yield_now(); }'
	expect fail 'fn f() { std::thread::park_timeout(d); }'
	expect fail 'fn f() { let n = std::thread::available_parallelism(); }'
	expect fail '#[cfg(any(test, target_arch = "wasm32"))]
fn f() { tokio::spawn(async {}); }'
	expect fail '#[cfg(feature = "encryption")]
fn f() { tokio::spawn(async {}); }'
	expect fail '#[cfg(not(feature = "encryption"))]
fn f() { tokio::spawn(async {}); }'
	expect fail '#[cfg(target_family = "wasm")]
fn f() { tokio::spawn(async {}); }'
	expect fail '#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
fn f() { tokio::spawn(async {}); }'
	expect fail '#[cfg(test)]
fn g() {}
fn f() { tokio::spawn(async {}); }'
	expect fail '#[cfg(test)]
mod tests {
	fn g() {}
}
fn f() { tokio::spawn(async {}); }'

	# These are not violations.
	expect pass 'fn f() { let _ = rt::Instant::now(); rt::spawn(async {}); rt::sleep(d).await; }'
	expect pass 'use crate::rt::Instant;
fn f() { let _ = Instant::now(); }'
	expect pass '// tokio::spawn is not called here, nor std::time::Instant::now
fn f() {}'
	expect pass '/// Not tokio::time::sleep(d), and not thread::sleep.
fn f() {}'
	expect pass 'fn f() { let _ = "tokio::spawn and std::time::SystemTime::now"; }'
	expect pass 'use tokio::sync::{Notify, oneshot};
use std::time::Duration;'
	expect pass 'fn f() { let _ = std::thread::JoinHandle::<()>::is_finished; }'
	expect pass '#[cfg(test)]
mod tests {
	fn f() { tokio::spawn(async {}); std::thread::sleep(d); let _ = std::time::Instant::now(); }
}
fn g() {}'
	expect pass '#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use std::time::{Duration, Instant};
	fn f() { tokio::time::sleep(d); }
}'
	expect pass '#[cfg(test)]
fn f() { tokio::spawn(async {}); }
fn g() {}'
	expect pass '#[cfg(test)]
use std::time::Instant;
fn g() {}'
	expect pass '#[cfg(test)]
#[allow(dead_code)]
fn f() {
	let s = "}";
	tokio::spawn(async {});
}
fn g() {}'
	expect pass '#[cfg(not(target_arch = "wasm32"))]
fn f() { std::thread::spawn(|| {}); }
fn g() {}'
	expect pass 'fn f() {
	#[cfg(not(target_arch = "wasm32"))]
	{
		if let Ok(handle) = tokio::runtime::Handle::try_current() {
			handle.block_on(fut);
		}
	}
}'
	expect pass '#[cfg(not(target_family = "wasm"))]
fn f() { std::thread::park_timeout(d); }'
	expect pass '#[cfg(unix)]
fn f() { std::thread::sleep(d); }'
	expect pass '#[cfg(not(any(target_os = "windows", target_family = "wasm")))]
fn f() { std::thread::sleep(d); }'
	expect pass '#[cfg(all(test, target_arch = "wasm32"))]
fn f() { std::thread::sleep(d); }'

	# A module file that a false cfg names, and the files below it, are not scanned; the shim is
	# not scanned; a file next to them is.
	fixture=$(mktemp -d)
	mkdir -p "$fixture/src/test" "$fixture/src/wal" "$fixture/src/wal/tests_dir"
	echo 'fn f() { tokio::spawn(async {}); }' > "$fixture/src/rt.rs"
	echo 'fn f() { tokio::spawn(async {}); }' > "$fixture/src/test/mod.rs"
	cat > "$fixture/src/lib.rs" <<'RS'
mod rt;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod test;
#[cfg(test)]
mod scrubber;
mod wal;
RS
	echo 'fn f() { tokio::spawn(async {}); }' > "$fixture/src/scrubber.rs"
	cat > "$fixture/src/wal/mod.rs" <<'RS'
#[cfg(test)]
mod tests;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
mod fine;
RS
	echo 'fn f() { thread::sleep(d); }' > "$fixture/src/wal/tests.rs"
	echo 'fn f() { thread::sleep(d); }' > "$fixture/src/wal/native.rs"
	echo 'fn f() {}' > "$fixture/src/wal/fine.rs"
	if ! scan_tree "$fixture" > /dev/null; then
		echo "self-test: files behind a false cfg or in src/rt.rs and src/test were scanned" >&2
		failures=$((failures + 1))
	fi
	echo 'fn f() { thread::sleep(d); }' > "$fixture/src/wal/fine.rs"
	out=$(scan_tree "$fixture" || true)
	case "$out" in
		*"src/wal/fine.rs:1:"*) ;;
		*)
			echo "self-test: a violation in a file that is part of the build was not reported: $out" >&2
			failures=$((failures + 1))
			;;
	esac
	rm -rf "$fixture"

	if [ "$failures" -ne 0 ]; then
		echo "self-test: $failures failure(s)" >&2
		return 1
	fi
	echo "check-rt-guard self-test: ok"
}

main() {
	case "${1:-}" in
		--self-test) self_test ;;
		*)
			if [ -n "${1:-}" ]; then
				root=$1
			else
				root=$(cd "$(dirname "$0")/.." && pwd)
			fi
			if scan_tree "$root"; then
				echo "check-rt-guard: ok"
			else
				echo >&2
				echo "check-rt-guard: the calls above must go through src/rt.rs (see the header of" >&2
				echo "scripts/check-rt-guard.sh for what is refused and what is exempt)." >&2
				return 1
			fi
			;;
	esac
}

main "$@"
