#!/bin/sh
# Regenerates CARGO.md, the readme shown on crates.io, from README.md.
#
# crates.io has no dark mode and cannot resolve repository-relative paths, so
# CARGO.md is README.md with three mechanical rewrites:
#   1. drop the dark-mode logo anchor and un-fragment the light-mode one
#   2. turn /img/ image paths into absolute raw.githubusercontent.com URLs
#   3. turn the relative LICENSE link into an absolute GitHub URL
#
# Edit README.md and run this (or `make readme`); never edit CARGO.md by hand.
# CI regenerates it and fails if the committed copy differs. The script also
# fails, without writing anything, if the result still holds a relative link,
# a relative image or a GitHub-only theme fragment, since none of them render
# on crates.io.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

awk \
	-v raw=https://raw.githubusercontent.com/surrealdb/surrealkv/main \
	-v blob=https://github.com/surrealdb/surrealkv/blob/main '
	/<a / && /#gh-dark-mode-only/ { skipping = 1 }
	skipping {
		if (/<\/a>/) skipping = 0
		next
	}
	{
		gsub(/#gh-light-mode-only/, "")
		gsub(/src="\/img\//, "src=\"" raw "/img/")
		gsub(/\]\(LICENSE\)/, "](" blob "/LICENSE)")
		print
	}
	END {
		if (skipping) {
			print "error: the dark-mode anchor in README.md is never closed" > "/dev/stderr"
			exit 1
		}
	}
' "$root/README.md" > "$tmp"

# Fenced code blocks are skipped: they may legitimately contain `](` or `src="`.
prose=$(awk '/^[[:space:]]*```/ { fenced = !fenced; next } !fenced' "$tmp")
absolute='(https?://|mailto:|#)'

problems=$(
	{
		printf '%s\n' "$prose" | grep -nE 'gh-(dark|light)-mode' || true
		printf '%s\n' "$prose" | grep -oE '\]\([^)]*\)' | grep -vE "^\]\\($absolute" || true
		printf '%s\n' "$prose" | grep -oE '(src|href)="[^"]*"' | grep -vE "^(src|href)=\"$absolute" || true
		printf '%s\n' "$prose" | grep -E '^\[[^]]+\]:[[:space:]]+' | grep -vE ":[[:space:]]+$absolute" || true
	}
)

if [ -n "$problems" ]; then
	echo "error: README.md would produce a CARGO.md that crates.io cannot render:" >&2
	printf '%s\n' "$problems" | sed 's/^/  /' >&2
	exit 1
fi

cp "$tmp" "$root/CARGO.md"
