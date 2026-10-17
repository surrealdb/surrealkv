#!/bin/sh
# Keeps the typed error of a file pool that has no free file from being turned into text.
#
# A pool answers a creation it cannot serve with `Error::FilePoolExhausted` (and, below the engine
# error, `wal::Error::PoolExhausted`), which the background tasks read as something that passes:
# the pool is refilled and the work is done again. Several places turn an error into text on its
# way up (`Error::Other(format!(..))`), and each of them already has an arm that lets an
# encryption error through unchanged, because a key that could not be had is not a disk that
# failed. The pool error needs the same arm at the same place, and a place that has one arm and
# not the other would turn a pool that is a moment away from having a file into an error that
# stops the database.
#
# What it checks, for each file of the table below that is ENFORCED: the number of passthrough
# arms for an encryption error (an arm whose body returns the error as it is) equals the number of
# passthrough arms for the pool error. An arm can be a line of its own, or one pattern of an or-
# pattern: `Error::Encryption(_) | Error::FilePoolExhausted => e`.
#
# A file that is PENDING is a place that has not been moved to the file system seam yet: its counts
# are reported, and the owner is the milestone that adds the arms. The table moves a file to
# ENFORCED in the change that adds its arms, and `--all` enforces every file at once.
#
# A source file that has an encryption passthrough arm and is not in the table fails the check, so
# that a new place is not forgotten, and so does a file of the table that does not exist.
#
# A second rule covers what the first cannot count. A pool that is at its limit of files, and a
# browser that is out of quota, give an I/O error of the kind `StorageFull` (`is_pool_full`,
# `is_storage_full`). It is an `Error::Io`, so no arm of the shape above lets it through, and each
# of the places above needs one more arm for it, right after the arm of the encryption error:
# `e if e.is_storage_full() => e`. A file of GUARDED below is checked line by line: after a
# passthrough arm for an encryption error (and the arms for the pool error that follow it), the
# next arm must be the guard for a full storage, or, in the rotation of the log (`rotation_error`),
# the arm that gives the same error its type. A guard that is deleted, or that is moved to another
# place, fails the check, which balancing the numbers of arms of a file does not see.
#
# It does not see a new wrapper that is not modelled on an encryption arm; the engine has a second
# safety net for that (the exhaustion witness of the background tasks).
#
# Usage:
#   scripts/check-passthrough.sh              check the src/ of this repository
#   scripts/check-passthrough.sh --all        enforce the pending files too
#   scripts/check-passthrough.sh DIR          check DIR/src (a copy of the layout, for the self-test)
#   scripts/check-passthrough.sh --self-test  run the script against fixtures that must fail and
#                                             fixtures that must pass
#
# POSIX sh, grep -E and awk only.
set -eu

# file|state|owner or reason
TABLE='src/error.rs|ENFORCED|conversions
src/storage/mod.rs|ENFORCED|log_error
src/wal/mod.rs|ENFORCED|wal::Error (no arm of the shape counted here; its io::Error downcast and the severity arm in error.rs are pinned by src/test/fs_error_tests.rs, not by this script)
src/lsm.rs|ENFORCED|memtable flush, rotation (rotation_error), manifest update, shutdown flush
src/ring/pipeline.rs|ENFORCED|the group failure, drop of an unapplied group, heal
src/levels/mod.rs|ENFORCED|check_manifest_before_open_in, manifest_write_error
src/vlog.rs|ENFORCED|the four resolve_* wrappers (reads: kept for the same shape)
src/checkpoint.rs|EXCLUDED|checkpoint and restore are refused on a pool
src/vlog/sealed.rs|EXCLUDED|a sealed value log is refused on a pool
src/inspect.rs|EXCLUDED|inspect takes a Path and is refused on a pool'

# The files whose passthrough arms for an encryption error are each followed by the guard for a full
# storage.
GUARDED='src/lsm.rs
src/ring/pipeline.rs
src/storage/mod.rs'

ENC='Error::Encryption\([a-z_]*\)\)?[[:space:]]*(\|[^=]*)?=>[[:space:]]*(e|error|err|Err\(e\.clone\(\)\)|(crate::)?(error::)?Error::Encryption\([a-z]+\)),?[[:space:]]*$'
POOL='(File)?PoolExhausted\)*[[:space:]]*(\|[^=]*)?=>[[:space:]]*(e|error|err|Err\(e\.clone\(\)\)|(crate::)?(error::)?Error::FilePoolExhausted),?[[:space:]]*$'

# The arm that follows the arms of the encryption error and the pool error: the guard for a storage
# that is full (an I/O error of the kind StorageFull that no operating system raised), as a match
# arm over an engine error, or the arm of the rotation of the log for the same error of the log.
GUARD='(Err\()?e\)?[[:space:]]+if[[:space:]]+e\.is_storage_full\(\)[[:space:]]*=>|Error::IO\([a-z_]+\)[[:space:]]+if[[:space:]].*StorageFull.*=>|wal::Error::StorageFull\([a-z_]+\)[[:space:]]*=>'

count() {
	n=$(grep -v '^[[:space:]]*//' "$2" | grep -Ec "$1" || true)
	echo "${n:-0}"
}

# unguarded FILE: prints the line number of every passthrough arm for an encryption error that is not
# followed by the guard for a full storage (after the arms for the pool error, which may be on the
# same line or on the next lines), and of one at the end of the file.
unguarded() {
	ENC_RE=$ENC POOL_RE=$POOL GUARD_RE=$GUARD awk '
		BEGIN { enc = ENVIRON["ENC_RE"]; pool = ENVIRON["POOL_RE"]; guard = ENVIRON["GUARD_RE"] }
		/^[[:space:]]*\/\// { next }
		{
			if (pending) {
				if ($0 ~ enc || $0 ~ pool) { next }
				if ($0 !~ guard) { print pending }
				pending = 0
			}
			if ($0 ~ enc) { pending = NR }
		}
		END { if (pending) { print pending } }
	' "$1"
}

# check ROOT ENFORCE_ALL: prints the table, returns 1 on a violation.
check_tree() {
	root=$1
	all=$2
	status=0
	listed=$(printf '%s\n' "$TABLE" | cut -d'|' -f1)

	printf '%s\n' "$TABLE" | while IFS='|' read -r file state note; do
		path="$root/$file"
		if [ ! -f "$path" ]; then
			if [ "${PASSTHROUGH_ALLOW_MISSING:-0}" = 1 ]; then
				continue
			fi
			echo "check-passthrough: $file is in the table and does not exist (renamed? update the table)" >&2
			echo MISSING >> "$root/.passthrough-status"
			continue
		fi
		[ "$state" = EXCLUDED ] && continue
		enc=$(count "$ENC" "$path")
		pool=$(count "$POOL" "$path")
		if [ "$state" = ENFORCED ] || [ "$all" = 1 ]; then
			if [ "$enc" != "$pool" ]; then
				echo "check-passthrough: $file has $enc passthrough arm(s) for an encryption error and $pool for FilePoolExhausted/PoolExhausted" >&2
				echo "    each 'Error::Encryption(_) => e'-shaped arm needs one for the pool error next to it" >&2
				echo BAD >> "$root/.passthrough-status"
			fi
		else
			echo "check-passthrough: pending $file: $enc encryption arm(s), $pool pool arm(s) ($note)"
		fi
	done

	# The arms for a full storage, in the files that have them.
	printf '%s\n' "$GUARDED" | while IFS= read -r file; do
		path="$root/$file"
		[ -n "$file" ] && [ -f "$path" ] || continue
		missing=$(unguarded "$path")
		if [ -n "$missing" ]; then
			echo "check-passthrough: $file has a passthrough arm for an encryption error with no guard for a full storage after it (line $(printf '%s' "$missing" | tr '\n' ' '))" >&2
			echo "    the next arm must be 'e if e.is_storage_full() => e', or its form for that place" >&2
			echo NOGUARD >> "$root/.passthrough-status"
		fi
	done

	# A place that is not in the table.
	find "$root/src" -name '*.rs' -not -path "$root/src/test/*" | sort | while IFS= read -r path; do
		rel=${path#"$root"/}
		if printf '%s\n' "$listed" | grep -qxF "$rel"; then
			continue
		fi
		if [ "$(count "$ENC" "$path")" != 0 ]; then
			echo "check-passthrough: $rel has an encryption passthrough arm and is not in the table of scripts/check-passthrough.sh" >&2
			echo UNLISTED >> "$root/.passthrough-status"
		fi
	done

	if [ -f "$root/.passthrough-status" ]; then
		rm -f "$root/.passthrough-status"
		return 1
	fi
	return $status
}

# --- self-test ------------------------------------------------------------------------------

# Builds a tree with $2 as src/error.rs (ENFORCED) and, if given, $3 as src/lsm.rs (PENDING), and
# says whether the check fails ("fail") or passes ("pass") with --all as $1.
verdict() {
	all=$1
	fixture=$(mktemp -d)
	mkdir -p "$fixture/src"
	printf '%s\n' "$2" > "$fixture/src/error.rs"
	if [ -n "${3:-}" ]; then printf '%s\n' "$3" > "$fixture/src/lsm.rs"; fi
	if [ -n "${4:-}" ]; then printf '%s\n' "$4" > "$fixture/src/other.rs"; fi
	if PASSTHROUGH_ALLOW_MISSING=1 check_tree "$fixture" "$all" > /dev/null 2>&1; then echo pass; else echo fail; fi
	rm -rf "$fixture"
}

expect() {
	want=$1
	got=$(verdict "$2" "$3" "${4:-}" "${5:-}")
	if [ "$got" != "$want" ]; then
		echo "self-test: expected $want, got $got, for (all=$2):" >&2
		printf '%s\n' "$3" "${4:-}" "${5:-}" | sed 's/^/    /' >&2
		failures=$((failures + 1))
	fi
}

self_test() {
	failures=0

	# The fixtures treat src/lsm.rs as a file that is still pending, which none of the real table
	# is any more: the table of the self-test says so, and the real one is put back after it.
	real_table=$TABLE
	TABLE=$(printf '%s\n' "$TABLE" | sed 's#^src/lsm.rs|ENFORCED|#src/lsm.rs|PENDING|#')
	# The guard for a full storage is enforced only where a fixture says so.
	real_guarded=$GUARDED
	GUARDED=

	# An arm of each shape that the engine has, with and without its pool arm.
	expect fail 0 '	Error::Encryption(_) => e,'
	expect pass 0 '	Error::Encryption(_) => e,
	Error::FilePoolExhausted => e,'
	expect fail 0 '	Error::Encryption(_) => error,'
	expect pass 0 '	Error::Encryption(_) => error,
	Error::FilePoolExhausted => error,'
	expect fail 0 '	Err(e @ Error::Encryption(_)) => Err(e.clone()),'
	expect pass 0 '	Err(e @ (Error::Encryption(_) | Error::FilePoolExhausted)) => Err(e.clone()),'
	expect pass 0 '	Err(e @ Error::Encryption(_)) => Err(e.clone()),
	Err(e @ Error::FilePoolExhausted) => Err(e.clone()),'
	expect fail 0 '	Error::Encryption(_) | Error::PipelineStall => e,'
	expect pass 0 '	Error::Encryption(_) | Error::FilePoolExhausted | Error::PipelineStall => e,'
	expect fail 0 '	crate::wal::Error::Encryption(err) => Error::Encryption(err),'
	expect pass 0 '	crate::wal::Error::Encryption(err) => Error::Encryption(err),
	crate::wal::Error::PoolExhausted => Error::FilePoolExhausted,'
	expect fail 0 '	wal::Error::Encryption(error) => Error::Encryption(error),'
	expect pass 0 '	wal::Error::Encryption(error) => Error::Encryption(error),
	wal::Error::PoolExhausted => Error::FilePoolExhausted,'
	# Two arms and one pool arm is still a missing arm.
	expect fail 0 '	Error::Encryption(_) => e,
	Error::Encryption(_) => e,
	Error::FilePoolExhausted => e,'
	# A pool arm with no encryption arm is also unbalanced, which says the table is out of date.
	expect fail 0 '	Error::FilePoolExhausted => e,'

	# Lines that are not passthrough arms are not counted.
	expect pass 0 '	Self::Encryption(err) => write!(f, "Encryption error: {err}"),
	(_, Error::Encryption(err)) => match err.class() {
	Err(err @ Error::Encryption(_)) => {
	matches!(self, Error::Encryption(_) | Error::IndexGap { .. })
	(_, Error::FilePoolExhausted) => ErrorSeverity::SoftError,
	// Error::Encryption(_) => e,'
	expect pass 0 'fn nothing() {}'

	# A pending file is reported and not enforced, until --all.
	expect pass 0 '	Error::Encryption(_) => e,
	Error::FilePoolExhausted => e,' '	Error::Encryption(_) => e,'
	expect fail 1 '	Error::Encryption(_) => e,
	Error::FilePoolExhausted => e,' '	Error::Encryption(_) => e,'
	expect pass 1 '	Error::Encryption(_) => e,
	Error::FilePoolExhausted => e,' '	Error::Encryption(_) => e,
	Error::FilePoolExhausted => e,'

	# The guard for a full storage after the arms of the encryption error and the pool error, in
	# the shapes that the engine has. The fixtures are the file src/lsm.rs, with every arm balanced
	# (--all), so that only the guard decides.
	GUARDED='src/lsm.rs'
	enc_pool='	Error::Encryption(_) | Error::FilePoolExhausted => e,'
	other_arm='	e => Error::Other(format!("Failed: {e}")),'
	# No guard: the next arm is another one. This is a guard that was deleted.
	expect fail 1 'fn nothing() {}' "$enc_pool
$other_arm"
	# The guard right after it.
	expect pass 1 'fn nothing() {}' "$enc_pool
	e if e.is_storage_full() => e,
$other_arm"
	# The arms on two lines, as the pool arm on a line of its own.
	expect pass 1 'fn nothing() {}' '	Error::Encryption(_) => e,
	Error::FilePoolExhausted => e,
	e if e.is_storage_full() => e,
	e => x,'
	expect fail 1 'fn nothing() {}' '	Error::Encryption(_) => e,
	Error::FilePoolExhausted => e,
	e => x,'
	# The shape of a result.
	expect pass 1 'fn nothing() {}' '	Err(e @ (Error::Encryption(_) | Error::FilePoolExhausted)) => Err(e.clone()),
	Err(e) if e.is_storage_full() => Err(e.clone()),
	Err(e) => Err(Error::Other(e)),'
	expect fail 1 'fn nothing() {}' '	Err(e @ (Error::Encryption(_) | Error::FilePoolExhausted)) => Err(e.clone()),
	Err(e) => Err(Error::Other(e)),'
	# A guard in another place does not do for this one.
	expect fail 1 'fn nothing() {}' "$enc_pool
$other_arm
}
fn elsewhere() {
	e if e.is_storage_full() => e,"
	# An arm that is the last line of the file has no guard after it.
	expect fail 1 'fn nothing() {}' "$enc_pool"
	# The rotation of the log has the same error of the log, with its own shape.
	expect pass 1 'fn nothing() {}' '	wal::Error::Encryption(error) => Error::Encryption(error),
	wal::Error::PoolExhausted => Error::FilePoolExhausted,
	wal::Error::IO(error) if pooled && error.kind() == std::io::ErrorKind::StorageFull => Error::Io(x),
	error => Error::Other(format!("{what}: {error}")),'
	expect fail 1 'fn nothing() {}' '	wal::Error::Encryption(error) => Error::Encryption(error),
	wal::Error::PoolExhausted => Error::FilePoolExhausted,
	error => Error::Other(format!("{what}: {error}")),'
	# Comments between the arms do not count as arms.
	expect pass 1 'fn nothing() {}' "$enc_pool
	// A full storage keeps its type too.
	e if e.is_storage_full() => e,
$other_arm"
	# A file that is not listed is not checked for the guard.
	GUARDED='src/other.rs'
	expect pass 1 'fn nothing() {}' "$enc_pool
$other_arm"
	GUARDED=

	# A place that is not in the table is refused.
	expect fail 0 'fn nothing() {}' '' '	Error::Encryption(_) => e,'
	expect pass 0 'fn nothing() {}' '' '	let x = 1;'

	# A file of the table that does not exist is refused, unless the fixture says it may not.
	fixture=$(mktemp -d)
	mkdir -p "$fixture/src"
	if check_tree "$fixture" 0 > /dev/null 2>&1; then
		echo "self-test: a missing file of the table was not refused" >&2
		failures=$((failures + 1))
	fi
	rm -rf "$fixture"

	TABLE=$real_table
	GUARDED=$real_guarded
	if [ "$failures" -ne 0 ]; then
		echo "self-test: $failures failure(s)" >&2
		return 1
	fi
	echo "check-passthrough self-test: ok"
}

main() {
	all=0
	root=
	for arg in "$@"; do
		case "$arg" in
			--self-test) self_test; return $? ;;
			--all) all=1 ;;
			*) root=$arg ;;
		esac
	done
	if [ -z "$root" ]; then
		root=$(cd "$(dirname "$0")/.." && pwd)
	fi
	if check_tree "$root" "$all"; then
		echo "check-passthrough: ok"
	else
		echo >&2
		echo "check-passthrough: see the header of scripts/check-passthrough.sh for what is checked." >&2
		return 1
	fi
}

main "$@"
