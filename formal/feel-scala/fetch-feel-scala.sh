#!/usr/bin/env bash
# Resolves the feel-scala engine jar Zeebe embeds at the pinned Zeebe commit and
# prints its local path. The version is DERIVED, never duplicated: it is read
# from `<version.feel-scala>` in the pinned camunda/camunda `parent/pom.xml`
# (repository + sha from formal/parity/zeebe-pin.json, the single Zeebe pin).
# The `-complete` (self-contained) jar is downloaded from Maven Central into a
# cache dir and verified against Central's published SHA-1. Idempotent; a cached
# jar is re-verified, not re-downloaded.
#
# Usage: formal/feel-scala/fetch-feel-scala.sh   (prints the jar path on stdout)
#   FEEL_SCALA_CACHE overrides the cache dir (default ~/.cache/nanobpm-feel-scala).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pin="$here/../parity/zeebe-pin.json"
cache="${FEEL_SCALA_CACHE:-$HOME/.cache/nanobpm-feel-scala}"

read -r repo sha < <(node -e '
  const p = JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"));
  const m = /^https:\/\/github\.com\/([^/]+\/[^/]+?)(\.git)?$/.exec(p.repository);
  if (!m || !/^[0-9a-f]{40}$/.test(p.sha)) { console.error("bad zeebe pin"); process.exit(1); }
  console.log(m[1], p.sha);' "$pin")

mkdir -p "$cache"
pom="$cache/parent-pom-$sha.xml"
if [ ! -s "$pom" ]; then
  curl -fsSL --retry 3 "https://raw.githubusercontent.com/$repo/$sha/parent/pom.xml" -o "$pom.tmp"
  mv "$pom.tmp" "$pom"
fi
version="$(sed -n 's|.*<version.feel-scala>\(.*\)</version.feel-scala>.*|\1|p' "$pom")"
if ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "FAIL: could not derive version.feel-scala from $repo@$sha parent/pom.xml (got '$version')" >&2
  exit 1
fi

base="https://repo1.maven.org/maven2/org/camunda/feel/feel-engine/$version/feel-engine-$version-complete.jar"
jar="$cache/feel-engine-$version-complete.jar"
if [ ! -s "$jar.sha1" ]; then
  curl -fsSL --retry 3 "$base.sha1" -o "$jar.sha1.tmp" && mv "$jar.sha1.tmp" "$jar.sha1"
fi
if [ ! -s "$jar" ]; then
  curl -fsSL --retry 3 "$base" -o "$jar.tmp" && mv "$jar.tmp" "$jar"
fi
want="$(cut -c1-40 "$jar.sha1")"
got="$( (command -v sha1sum >/dev/null && sha1sum "$jar" || shasum -a 1 "$jar") | cut -c1-40)"
if [ "$want" != "$got" ]; then
  rm -f "$jar"
  echo "FAIL: feel-engine-$version-complete.jar SHA-1 mismatch (want $want, got $got)" >&2
  exit 1
fi
echo "feel-scala $version (from $repo@$sha)" >&2
echo "$jar"
