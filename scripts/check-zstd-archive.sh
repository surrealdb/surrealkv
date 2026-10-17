#!/bin/sh
# Fails if a `libzstd.a` that a WebAssembly build produced has no members.
#
# The browser tests link the whole engine, and with it zstd's C code. That is compiled by clang
# and archived by the archiver of the target. GNU `ar` and Apple's write no symbol index for
# WebAssembly objects: Apple's writes a tiny archive with no member at all, and rust-lld then
# reports every zstd symbol as undefined. A link error says that late and badly, and a cached
# `target` directory can hold such an archive for as long as the cache lives. This script looks
# at the archives themselves.
#
# Usage:
#   scripts/check-zstd-archive.sh [--allow-none] [dir]
#                                         dir is searched for `libzstd.a` (default: target).
#                                         Fails if one is under 1000 bytes, and also if there is
#                                         none, unless --allow-none is given (a cache that
#                                         restores the compiled crates may not restore the build
#                                         directory that held the archive).
#   scripts/check-zstd-archive.sh --self-test
#                                         checks the check, on fixtures: a 96 byte archive and an
#                                         empty directory must fail, a larger archive must pass.
#
# The size is read with `wc -c`, because the flags of `stat` differ between macOS and Linux.
set -eu

min_bytes=1000
allow_none=0

check() {
	dir=$1
	if [ ! -d "$dir" ]; then
		echo "check-zstd-archive: $dir is not a directory" >&2
		return 1
	fi
	found=0
	bad=0
	# A file name may hold a space: read whole lines, never split words.
	archives=$(find "$dir" -name libzstd.a -type f | sort)
	if [ -n "$archives" ]; then
		old_ifs=$IFS
		IFS='
'
		for archive in $archives; do
			found=$((found + 1))
			size=$(wc -c < "$archive" | tr -d ' ')
			if [ "$size" -lt "$min_bytes" ]; then
				echo "check-zstd-archive: $archive is $size bytes, so it has no member:" >&2
				echo "  the archiver of the wasm32 build wrote no symbol index. Set" >&2
				echo "  AR_wasm32_unknown_unknown to LLVM's llvm-ar (and CC_wasm32_unknown_unknown" >&2
				echo "  to clang), and remove the stale archive or its build directory." >&2
				bad=1
			fi
		done
		IFS=$old_ifs
	fi
	if [ "$found" -eq 0 ]; then
		if [ "$allow_none" -eq 1 ]; then
			echo "check-zstd-archive: no libzstd.a under $dir, so there is nothing to check"
			return 0
		fi
		echo "check-zstd-archive: no libzstd.a under $dir: the engine was not linked there, or" >&2
		echo "  the build moved the archive, and then nothing checks it." >&2
		return 1
	fi
	if [ "$bad" -eq 0 ]; then
		echo "check-zstd-archive: $found libzstd.a under $dir, none under $min_bytes bytes"
	fi
	return "$bad"
}

self_test() {
	tmp=$(mktemp -d)
	trap 'rm -rf "$tmp"' EXIT
	failed=0

	# The archive of Apple's `ar`: the magic and nothing else, padded to its observed size.
	mkdir -p "$tmp/bad/build/zstd-sys-1/out"
	printf '!<arch>\n' > "$tmp/bad/build/zstd-sys-1/out/libzstd.a"
	head -c 88 /dev/zero >> "$tmp/bad/build/zstd-sys-1/out/libzstd.a"

	mkdir -p "$tmp/good/build/zstd-sys-1/out"
	printf '!<arch>\n' > "$tmp/good/build/zstd-sys-1/out/libzstd.a"
	head -c 4096 /dev/zero >> "$tmp/good/build/zstd-sys-1/out/libzstd.a"

	# One good archive does not excuse a bad one in another build directory.
	mkdir -p "$tmp/mixed/a/out" "$tmp/mixed/b/out"
	cp "$tmp/good/build/zstd-sys-1/out/libzstd.a" "$tmp/mixed/a/out/libzstd.a"
	cp "$tmp/bad/build/zstd-sys-1/out/libzstd.a" "$tmp/mixed/b/out/libzstd.a"

	mkdir -p "$tmp/empty"

	if [ "$(wc -c < "$tmp/bad/build/zstd-sys-1/out/libzstd.a" | tr -d ' ')" -ne 96 ]; then
		echo "check-zstd-archive: self-test: the fixture is not 96 bytes" >&2
		failed=1
	fi
	if check "$tmp/bad" > /dev/null 2>&1; then
		echo "check-zstd-archive: self-test: a 96 byte archive passed" >&2
		failed=1
	fi
	if ! check "$tmp/good" > /dev/null 2>&1; then
		echo "check-zstd-archive: self-test: a 4096 byte archive failed" >&2
		failed=1
	fi
	if check "$tmp/mixed" > /dev/null 2>&1; then
		echo "check-zstd-archive: self-test: a bad archive next to a good one passed" >&2
		failed=1
	fi
	if check "$tmp/empty" > /dev/null 2>&1; then
		echo "check-zstd-archive: self-test: a directory with no archive passed" >&2
		failed=1
	fi
	allow_none=1
	if ! check "$tmp/empty" > /dev/null 2>&1; then
		echo "check-zstd-archive: self-test: --allow-none failed on a directory with no archive" >&2
		failed=1
	fi
	if check "$tmp/bad" > /dev/null 2>&1; then
		echo "check-zstd-archive: self-test: --allow-none passed a 96 byte archive" >&2
		failed=1
	fi
	allow_none=0
	if check "$tmp/missing" > /dev/null 2>&1; then
		echo "check-zstd-archive: self-test: a directory that does not exist passed" >&2
		failed=1
	fi
	if [ "$failed" -eq 0 ]; then
		echo "check-zstd-archive: self-test passed"
	fi
	return "$failed"
}

if [ "${1:-}" = "--allow-none" ]; then
	allow_none=1
	shift
fi

case "${1:-}" in
	--self-test) self_test ;;
	-h | --help)
		sed -n '2,/^set -eu/p' "$0" | sed '$d'
		;;
	*) check "${1:-target}" ;;
esac
