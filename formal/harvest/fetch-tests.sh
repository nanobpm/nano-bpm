#!/usr/bin/env bash
# Sparse-fetch the Zeebe engine-test sources the corpus is harvested from, at the
# commit pinned in zeebe-test-pin.json, and print the checkout directory.
#
# Usage: formal/harvest/fetch-tests.sh [dest]   (default: ~/.cache/nanobpm-harvest/<sha>)
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
pin="$here/zeebe-test-pin.json"
field() { node -e "process.stdout.write(String(require(process.argv[1])[process.argv[2]]))" "$pin" "$1"; }
repo="$(field repository)"
sha="$(field sha)"
dest="${1:-${XDG_CACHE_HOME:-$HOME/.cache}/nanobpm-harvest/$sha}"

paths="$(node -e "for (const p of require(process.argv[1]).testPaths) console.log('/' + p)" "$pin")"

# The completion marker records the repository, sha AND sparse paths, so a pin
# edit to any of them refetches. Keying on paths alone would return a stale
# checkout when the sha/repository changes under a caller-supplied fixed dest
# (the default dest is already sha-keyed, but a fixed dest is not).
marker="$(printf 'repository=%s\nsha=%s\npaths:\n%s' "$repo" "$sha" "$paths")"
if [ -f "$dest/.complete" ] && [ "$(cat "$dest/.complete")" = "$marker" ]; then
  echo "$dest"
  exit 0
fi

rm -rf "$dest"
mkdir -p "$dest"
git -C "$dest" init -q
git -C "$dest" remote add origin "$repo"
printf '%s\n' "$paths" | git -C "$dest" sparse-checkout set --no-cone --stdin
git -C "$dest" fetch -q --depth 1 --filter=blob:none origin "$sha"
git -C "$dest" checkout -q FETCH_HEAD
printf '%s' "$marker" > "$dest/.complete"
echo "$dest"
